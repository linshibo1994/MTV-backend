"""视频远端取消结果的持久化契约与终态 CAS。"""

from __future__ import annotations

import hashlib
import hmac
import re
from copy import deepcopy
from typing import Any, cast

from sqlalchemy import select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import noload

from app.models.video_generation_job import VideoGenerationJob
from app.providers.video_gen import (
    VideoCancellationOutcome,
    VideoCancellationResult,
    VideoTaskCheckpointConflict,
    VideoTaskCheckpointUnavailable,
)

PROVIDER_CANCELLATION_KEY = "provider_cancellation"
PROVIDER_SUBMISSION_KEY = "provider_submission"
PROVIDER_TASK_CHECKPOINT_LEASE_KEY = "provider_task_checkpoint_lease"
ORPHAN_PROVIDER_CANCELLATIONS_KEY = "orphan_provider_cancellations"
_NO_ACTIVE_CHECKPOINT_LEASE = "none"
_MAX_ORPHAN_GENERATIONS = 16
_MAX_ORPHAN_TASKS_PER_GENERATION = 8
_MAX_ORPHAN_TASKS_TOTAL = 64
_MAX_CANCELLATION_DETAIL_LENGTH = 512
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_SENSITIVE_PAIR = re.compile(
    r"(?i)\b(?:api[_-]?key|authorization|password|secret|token)\s*[:=]\s*[^\s,;]+"
)
_URL = re.compile(r"(?i)https?://[^\s]+")
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]+")
_UNRESOLVED_SUBMISSION_STATES = frozenset({"submit_intent", "submission_uncertain"})
_SUBMISSION_STATE_RANK = {
    "submit_intent": 1,
    "submission_uncertain": 2,
    "submit_rejected": 3,
}


def pending_provider_cancellation(provider_task_id: str) -> dict[str, Any]:
    """构造与本地 canceled 同事务提交的待处理取消事实。"""
    return {
        "provider_task_id": provider_task_id,
        "outcome": "pending",
        "detail": None,
        "retryable": True,
        "attempts": 0,
    }


def merge_pending_provider_cancellation(
    output_json: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
    provider_task_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    record = pending_provider_cancellation(provider_task_id)
    return _merge_provider_cancellation(output_json, metrics, provider_task_id, record)


def video_task_lease_generation(lease_token: str | None) -> str:
    """生成不泄露原始 lease 的稳定执行代际标识。"""

    if not lease_token:
        return _NO_ACTIVE_CHECKPOINT_LEASE
    digest = hashlib.sha256(lease_token.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _provider_task_identity(provider_task_id: str) -> str:
    digest = hashlib.sha256(provider_task_id.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def merge_video_task_checkpoint_lease_authority(
    output_json: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
    lease_token: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """取消清空 worker lease 前，单调持久化触发取消的执行代际。"""

    authority = video_task_lease_generation(lease_token)
    merged_output = dict(output_json or {})
    merged_metrics = dict(metrics or {})
    existing = {
        str(value)
        for value in (
            merged_output.get(PROVIDER_TASK_CHECKPOINT_LEASE_KEY),
            merged_metrics.get(PROVIDER_TASK_CHECKPOINT_LEASE_KEY),
        )
        if value is not None and str(value)
    }
    if existing and existing != {authority}:
        raise _checkpoint_conflict("", "cancel_lease_authority")
    merged_output[PROVIDER_TASK_CHECKPOINT_LEASE_KEY] = authority
    merged_metrics[PROVIDER_TASK_CHECKPOINT_LEASE_KEY] = authority
    return merged_output, merged_metrics


def _canceled_checkpoint_lease_matches(
    output_json: dict[str, Any],
    metrics: dict[str, Any],
    lease_token: str,
) -> bool:
    output_authority = output_json.get(PROVIDER_TASK_CHECKPOINT_LEASE_KEY)
    metrics_authority = metrics.get(PROVIDER_TASK_CHECKPOINT_LEASE_KEY)
    if (
        not isinstance(output_authority, str)
        or not isinstance(metrics_authority, str)
        or output_authority == _NO_ACTIVE_CHECKPOINT_LEASE
    ):
        return False
    return hmac.compare_digest(output_authority, metrics_authority) and hmac.compare_digest(
        output_authority,
        video_task_lease_generation(lease_token),
    )


def _checkpoint_conflict(
    provider_task_id: str,
    reason: str,
) -> VideoTaskCheckpointConflict:
    return VideoTaskCheckpointConflict(
        f"video_task_checkpoint_conflict:{reason}",
        provider_task_id=provider_task_id,
    )


class OrphanProviderCancellationCapacityExceeded(RuntimeError):
    """旧代际事实已达上限，调用方必须停止继续 reclaim。"""


def _orphan_cancellation_state(
    output_json: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
    *,
    provider_task_id: str = "",
) -> dict[str, Any]:
    output_state = (output_json or {}).get(ORPHAN_PROVIDER_CANCELLATIONS_KEY)
    metrics_state = (metrics or {}).get(ORPHAN_PROVIDER_CANCELLATIONS_KEY)
    if output_state is not None and not isinstance(output_state, dict):
        raise _checkpoint_conflict(provider_task_id, "orphan_output")
    if metrics_state is not None and not isinstance(metrics_state, dict):
        raise _checkpoint_conflict(provider_task_id, "orphan_metrics")
    if (
        isinstance(output_state, dict)
        and isinstance(metrics_state, dict)
        and output_state != metrics_state
    ):
        raise _checkpoint_conflict(provider_task_id, "orphan_mirror")
    raw_state = output_state if isinstance(output_state, dict) else metrics_state
    if raw_state is None:
        return {"pending": False, "generations": {}}
    generations = raw_state.get("generations")
    if not isinstance(raw_state.get("pending"), bool) or not isinstance(generations, dict):
        raise _checkpoint_conflict(provider_task_id, "orphan_shape")
    normalized = deepcopy(raw_state)
    for generation, generation_record in generations.items():
        if (
            not isinstance(generation, str)
            or not generation.startswith("sha256:")
            or not isinstance(generation_record, dict)
            or not isinstance(generation_record.get("tasks"), dict)
        ):
            raise _checkpoint_conflict(provider_task_id, "orphan_generation")
    return normalized


def _orphan_state_has_pending_tasks(state: dict[str, Any]) -> bool:
    generations = state.get("generations")
    if not isinstance(generations, dict):
        return False
    return any(
        isinstance(record, dict) and record.get("retryable") is True
        for generation_record in generations.values()
        if isinstance(generation_record, dict)
        for record in (
            generation_record.get("tasks", {}).values()
            if isinstance(generation_record.get("tasks"), dict)
            else ()
        )
    )


def _write_orphan_cancellation_state(
    output_json: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
    state: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized = deepcopy(state)
    normalized["pending"] = _orphan_state_has_pending_tasks(normalized)
    merged_output = dict(output_json or {})
    merged_metrics = dict(metrics or {})
    merged_output[ORPHAN_PROVIDER_CANCELLATIONS_KEY] = deepcopy(normalized)
    merged_metrics[ORPHAN_PROVIDER_CANCELLATIONS_KEY] = deepcopy(normalized)
    return merged_output, merged_metrics


def merge_reclaimed_video_task_generation(
    output_json: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
    lease_token: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """清除 stale lease 前，持久化可识别迟到 ID 的旧代际墓碑。"""

    generation = video_task_lease_generation(lease_token)
    if generation == _NO_ACTIVE_CHECKPOINT_LEASE:
        raise _checkpoint_conflict("", "orphan_lease")
    state = _orphan_cancellation_state(output_json, metrics)
    generations = state["generations"]
    if generation not in generations:
        if len(generations) >= _MAX_ORPHAN_GENERATIONS:
            raise OrphanProviderCancellationCapacityExceeded(
                "orphan_provider_cancellation_generation_capacity"
            )
        generations[generation] = {"tasks": {}}
    return _write_orphan_cancellation_state(output_json, metrics, state)


def provider_submission_is_unresolved(
    output_json: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
) -> bool:
    """没有权威远端 ID 时，提交意图或响应不确定均禁止创建新 attempt。"""

    authority_consistent, provider_task_id = provider_task_id_authority(
        output_json,
        metrics,
    )
    if not authority_consistent:
        return True
    if provider_task_id is not None:
        return False
    output_has_record = PROVIDER_SUBMISSION_KEY in (output_json or {})
    metrics_has_record = PROVIDER_SUBMISSION_KEY in (metrics or {})
    if not output_has_record and not metrics_has_record:
        return False
    output_record = (output_json or {}).get(PROVIDER_SUBMISSION_KEY)
    metrics_record = (metrics or {}).get(PROVIDER_SUBMISSION_KEY)
    if (
        not isinstance(output_record, dict)
        or not isinstance(metrics_record, dict)
        or output_record != metrics_record
    ):
        return True
    state = str(output_record.get("state") or "")
    if state not in _SUBMISSION_STATE_RANK:
        return True
    raw_attempt = output_record.get("attempt")
    if raw_attempt is None:
        return True
    try:
        attempt = int(raw_attempt)
    except (TypeError, ValueError):
        return True
    if not str(output_record.get("provider_key") or "") or attempt <= 0:
        return True
    return state in _UNRESOLVED_SUBMISSION_STATES


def provider_submission_requires_manual_review(
    output_json: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
    *,
    provider_key: str,
    attempt: int,
) -> bool:
    """仅将完整、镜像一致且无远端 ID 的提交不确定记录转入人工核对。"""

    if not provider_key or attempt <= 0:
        return False
    authority_consistent, provider_task_id = provider_task_id_authority(output_json, metrics)
    if not authority_consistent or provider_task_id is not None:
        return False
    output_record = (output_json or {}).get(PROVIDER_SUBMISSION_KEY)
    metrics_record = (metrics or {}).get(PROVIDER_SUBMISSION_KEY)
    if (
        not isinstance(output_record, dict)
        or not isinstance(metrics_record, dict)
        or output_record != metrics_record
        or output_record.get("state") != "submission_uncertain"
        or str(output_record.get("provider_key") or "") != provider_key
    ):
        return False
    raw_attempt = output_record.get("attempt")
    if isinstance(raw_attempt, bool) or not isinstance(raw_attempt, int | str):
        return False
    try:
        return int(raw_attempt) == attempt
    except (TypeError, ValueError):
        return False


def _merge_provider_submission_state(
    output_json: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
    *,
    state: str,
    provider_key: str,
    attempt: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if state not in _SUBMISSION_STATE_RANK or not provider_key or attempt <= 0:
        raise _checkpoint_conflict("", "submission_payload")
    merged_output = dict(output_json or {})
    merged_metrics = dict(metrics or {})
    existing_records = [
        record
        for record in (
            merged_output.get(PROVIDER_SUBMISSION_KEY),
            merged_metrics.get(PROVIDER_SUBMISSION_KEY),
        )
        if isinstance(record, dict)
    ]
    if len(existing_records) == 2 and existing_records[0] != existing_records[1]:
        raise _checkpoint_conflict("", "submission_authority")
    existing = existing_records[0] if existing_records else None
    if existing is not None:
        if (
            str(existing.get("provider_key") or "") != provider_key
            or int(existing.get("attempt") or 0) != attempt
        ):
            raise _checkpoint_conflict("", "submission_identity")
        existing_state = str(existing.get("state") or "")
        if existing_state not in _SUBMISSION_STATE_RANK:
            raise _checkpoint_conflict("", "submission_state")
        if _SUBMISSION_STATE_RANK[existing_state] > _SUBMISSION_STATE_RANK[state]:
            raise _checkpoint_conflict("", "submission_state_regression")
    record = {
        "state": state,
        "provider_key": provider_key,
        "attempt": attempt,
    }
    merged_output[PROVIDER_SUBMISSION_KEY] = dict(record)
    merged_metrics[PROVIDER_SUBMISSION_KEY] = dict(record)
    return merged_output, merged_metrics


async def persist_provider_submission_state(
    db: Any,
    job_id: str,
    lease_token: str,
    *,
    state: str,
    provider_key: str,
    attempt: int,
) -> bool:
    """以当前 processing lease 单调持久化提交边界状态。"""

    job = (
        await db.execute(
            select(VideoGenerationJob)
            .where(VideoGenerationJob.job_id == job_id)
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if job is None:
        raise _checkpoint_conflict("", "submission_job_missing")
    if (
        job.status != "processing"
        or job.lease_token != lease_token
        or str(job.provider_key or "") != provider_key
        or int(job.attempt or 0) != attempt
    ):
        raise _checkpoint_conflict("", "submission_lease")

    current_output = dict(job.output_json or {})
    current_metrics = dict(job.metrics or {})
    authority_consistent, provider_task_id = provider_task_id_authority(
        current_output,
        current_metrics,
    )
    if not authority_consistent:
        raise _checkpoint_conflict("", "submission_task_authority")
    if provider_task_id is not None:
        raise _checkpoint_conflict(provider_task_id, "submission_already_identified")
    output_json, metrics = _merge_provider_submission_state(
        current_output,
        current_metrics,
        state=state,
        provider_key=provider_key,
        attempt=attempt,
    )
    if output_json == current_output and metrics == current_metrics:
        return True

    expected_version = int(job.version or 0)
    values: dict[str, Any] = {
        "output_json": output_json,
        "metrics": metrics,
        "version": VideoGenerationJob.version + 1,
    }
    if state == "submit_intent":
        values.update(
            progress_stage="submitting",
            progress_message="正在提交视频生成任务",
        )
    elif state == "submission_uncertain":
        values.update(
            progress_stage="submission_uncertain",
            progress_message="远端提交结果待人工核对",
        )
    updated = cast(
        CursorResult[Any],
        await db.execute(
            update(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job_id,
                VideoGenerationJob.status == "processing",
                VideoGenerationJob.lease_token == lease_token,
                VideoGenerationJob.version == expected_version,
            )
            .values(**values)
        ),
    )
    await db.flush()
    if updated.rowcount != 1:
        raise _checkpoint_conflict("", "submission_cas")
    return True


def _merge_orphan_video_task_checkpoint(
    output_json: dict[str, Any],
    metrics: dict[str, Any],
    provider_task_id: str,
    lease_token: str,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    state = _orphan_cancellation_state(
        output_json,
        metrics,
        provider_task_id=provider_task_id,
    )
    generation = video_task_lease_generation(lease_token)
    generations = state["generations"]
    generation_record = generations.get(generation)
    if not isinstance(generation_record, dict):
        return None
    tasks = generation_record.get("tasks")
    if not isinstance(tasks, dict):
        raise _checkpoint_conflict(provider_task_id, "orphan_tasks")
    task_key = _provider_task_identity(provider_task_id)
    existing = tasks.get(task_key)
    if existing is not None:
        if (
            not isinstance(existing, dict)
            or str(existing.get("provider_task_id") or "") != provider_task_id
        ):
            raise _checkpoint_conflict(provider_task_id, "orphan_task_identity")
        return _write_orphan_cancellation_state(output_json, metrics, state)

    total_tasks = sum(
        len(record.get("tasks", {}))
        for record in generations.values()
        if isinstance(record, dict) and isinstance(record.get("tasks"), dict)
    )
    if (
        len(tasks) >= _MAX_ORPHAN_TASKS_PER_GENERATION
        or total_tasks >= _MAX_ORPHAN_TASKS_TOTAL
    ):
        raise VideoTaskCheckpointUnavailable(
            "video_task_checkpoint_orphan_capacity",
            provider_task_id=provider_task_id,
        )
    tasks[task_key] = pending_provider_cancellation(provider_task_id)
    return _write_orphan_cancellation_state(output_json, metrics, state)


def _merge_canceled_video_task_checkpoint(
    output_json: dict[str, Any],
    metrics: dict[str, Any],
    provider_task_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """取消竞态只补 ID 与待取消事实，不直接调用 Provider。"""

    output_record = output_json.get(PROVIDER_CANCELLATION_KEY)
    metrics_record = metrics.get(PROVIDER_CANCELLATION_KEY)
    existing_records = [
        record
        for record in (output_record, metrics_record)
        if isinstance(record, dict)
    ]
    if any(
        str(record.get("provider_task_id") or "") != provider_task_id
        for record in existing_records
    ):
        raise _checkpoint_conflict(provider_task_id, "cancellation_record")
    record = (
        dict(existing_records[0])
        if existing_records
        else pending_provider_cancellation(provider_task_id)
    )
    return _merge_provider_cancellation(
        output_json,
        metrics,
        provider_task_id,
        record,
    )


async def persist_video_task_checkpoint(
    db: Any,
    job_id: str,
    provider_task_id: str,
    lease_token: str,
) -> bool:
    """以行锁和 version CAS 持久化远端任务 ID。

    processing 只接受当前 lease；已登记的 stale 代际只追加 orphan cancellation debt，
    不覆盖当前权威 ID。canceled 当前代际补 pending cancellation debt；其余状态仅允许
    已完整持久化的同 ID 重放或已登记旧代际落债。
    """

    if not provider_task_id or not lease_token:
        raise _checkpoint_conflict(provider_task_id, "invalid_payload")
    job = (
        await db.execute(
            select(VideoGenerationJob)
            .where(VideoGenerationJob.job_id == job_id)
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if job is None:
        raise _checkpoint_conflict(provider_task_id, "job_missing")

    current_output = dict(job.output_json or {})
    current_metrics = dict(job.metrics or {})
    authority_consistent, known_task_id = provider_task_id_authority(
        current_output,
        current_metrics,
    )
    if not authority_consistent:
        raise _checkpoint_conflict(provider_task_id, "authority")

    output_complete = current_output.get("provider_task_id") == provider_task_id
    metrics_complete = current_metrics.get("provider_task_id") == provider_task_id
    status = str(job.status or "")
    expected_version = int(job.version or 0)
    conditions = [
        VideoGenerationJob.job_id == job_id,
        VideoGenerationJob.status == status,
        VideoGenerationJob.version == expected_version,
    ]

    orphan_checkpoint: tuple[dict[str, Any], dict[str, Any]] | None = None
    current_processing_lease = status == "processing" and job.lease_token == lease_token
    if not current_processing_lease and known_task_id != provider_task_id:
        orphan_checkpoint = _merge_orphan_video_task_checkpoint(
            current_output,
            current_metrics,
            provider_task_id,
            lease_token,
        )
    if orphan_checkpoint is not None:
        output_json, metrics = orphan_checkpoint
        if output_json == current_output and metrics == current_metrics:
            return True
    elif known_task_id is not None and known_task_id != provider_task_id:
        raise _checkpoint_conflict(provider_task_id, "authority")

    elif status == "processing":
        if job.lease_token != lease_token:
            if output_complete and metrics_complete:
                return True
            raise _checkpoint_conflict(provider_task_id, "lease")
        if output_complete and metrics_complete:
            return True
        output_json = dict(current_output)
        metrics = dict(current_metrics)
        output_json["provider_task_id"] = provider_task_id
        metrics["provider_task_id"] = provider_task_id
        conditions.append(VideoGenerationJob.lease_token == lease_token)
    elif status == "canceled":
        output_json, metrics = _merge_canceled_video_task_checkpoint(
            current_output,
            current_metrics,
            provider_task_id,
        )
        if output_json == current_output and metrics == current_metrics:
            return True
        if not _canceled_checkpoint_lease_matches(
            current_output,
            current_metrics,
            lease_token,
        ):
            raise _checkpoint_conflict(provider_task_id, "lease")
    elif output_complete and metrics_complete:
        return True
    else:
        raise _checkpoint_conflict(provider_task_id, f"status_{status or 'missing'}")

    updated = cast(
        CursorResult[Any],
        await db.execute(
            update(VideoGenerationJob)
            .where(*conditions)
            .values(
                output_json=output_json,
                metrics=metrics,
                version=VideoGenerationJob.version + 1,
            )
        ),
    )
    await db.flush()
    if updated.rowcount != 1:
        raise _checkpoint_conflict(provider_task_id, "cas")
    return True


async def ensure_pending_provider_cancellation(
    db: Any,
    job_id: str,
    provider_task_id: str,
) -> bool:
    """确保 canceled 任务持有同一远端任务的可恢复取消事实。"""

    job = (
        await db.execute(
            select(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job_id,
                VideoGenerationJob.status == "canceled",
            )
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if job is None:
        return False
    current_output = dict(job.output_json or {})
    current_metrics = dict(job.metrics or {})
    authority_consistent, known_task_id = provider_task_id_authority(
        current_output,
        current_metrics,
    )
    if not authority_consistent or (
        known_task_id is not None and known_task_id != provider_task_id
    ):
        return False
    existing_output = current_output.get(PROVIDER_CANCELLATION_KEY)
    existing_metrics = current_metrics.get(PROVIDER_CANCELLATION_KEY)
    output_record = existing_output if isinstance(existing_output, dict) else None
    metrics_record = existing_metrics if isinstance(existing_metrics, dict) else None
    existing_records = [
        record for record in (output_record, metrics_record) if record is not None
    ]
    if any(
        str(record.get("provider_task_id") or "") != provider_task_id
        for record in existing_records
    ):
        return False
    if output_record is not None and metrics_record is not None:
        if output_record != metrics_record:
            return False
        if (
            current_output.get("provider_task_id") == provider_task_id
            and current_metrics.get("provider_task_id") == provider_task_id
        ):
            return True
    if existing_records:
        output_json, metrics = _merge_provider_cancellation(
            current_output,
            current_metrics,
            provider_task_id,
            existing_records[0],
        )
    else:
        output_json, metrics = merge_pending_provider_cancellation(
            current_output,
            current_metrics,
            provider_task_id,
        )
    expected_version = int(job.version or 0)
    updated = cast(
        CursorResult[Any],
        await db.execute(
            update(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job_id,
                VideoGenerationJob.status == "canceled",
                VideoGenerationJob.version == expected_version,
            )
            .values(
                output_json=output_json,
                metrics=metrics,
                version=VideoGenerationJob.version + 1,
            )
        ),
    )
    await db.flush()
    return updated.rowcount == 1


def sanitize_provider_cancellation_detail(detail: str | None) -> str | None:
    """删除 URL、凭据形态与控制字符，并限制持久化长度。"""
    if not detail:
        return None
    value = _CONTROL_CHARS.sub(" ", str(detail))
    value = _BEARER_TOKEN.sub("Bearer [REDACTED]", value)
    value = _SENSITIVE_PAIR.sub("[REDACTED]", value)
    value = _URL.sub("[URL]", value)
    value = " ".join(value.split())
    return value[:_MAX_CANCELLATION_DETAIL_LENGTH] or None


def provider_cancellation_record(
    result: VideoCancellationResult,
    *,
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    attempts = _safe_attempts(previous) + 1
    candidate = {
        "provider_task_id": result.provider_task_id,
        "outcome": result.outcome.value,
        "detail": sanitize_provider_cancellation_detail(result.detail),
        "retryable": result.outcome is VideoCancellationOutcome.FAILED,
        "attempts": attempts,
    }
    if _confirmed_result_must_be_preserved(previous, result):
        preserved = dict(previous or {})
        preserved["attempts"] = attempts
        return preserved
    return candidate


async def persist_provider_cancellation_result(
    db: Any,
    job_id: str,
    result: VideoCancellationResult,
) -> bool:
    """仅在 canceled 终态上按 version CAS 写入远端取消结果。"""
    job = (
        await db.execute(
            select(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job_id,
                VideoGenerationJob.status == "canceled",
            )
            .options(noload(VideoGenerationJob.script))
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if job is None:
        return False
    return await persist_locked_provider_cancellation_result(db, job, result)


async def persist_locked_provider_cancellation_result(
    db: Any,
    job: VideoGenerationJob,
    result: VideoCancellationResult,
) -> bool:
    """更新已加锁 canceled 行；status 与 version 同时参与 CAS。"""
    current_output = dict(job.output_json or {})
    current_metrics = dict(job.metrics or {})
    authority_consistent, known_task_id = provider_task_id_authority(
        current_output,
        current_metrics,
    )
    if not authority_consistent or (
        known_task_id is not None and known_task_id != result.provider_task_id
    ):
        return False
    previous = current_output.get(PROVIDER_CANCELLATION_KEY)
    if not isinstance(previous, dict):
        previous = current_metrics.get(PROVIDER_CANCELLATION_KEY)
    if not isinstance(previous, dict):
        previous = None
    record = provider_cancellation_record(result, previous=previous)
    output_json, metrics = _merge_provider_cancellation(
        current_output,
        current_metrics,
        result.provider_task_id,
        record,
    )
    expected_version = int(job.version or 0)
    updated = cast(
        CursorResult[Any],
        await db.execute(
            update(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job.job_id,
                VideoGenerationJob.status == "canceled",
                VideoGenerationJob.version == expected_version,
            )
            .values(
                output_json=output_json,
                metrics=metrics,
                version=VideoGenerationJob.version + 1,
            )
        ),
    )
    await db.flush()
    return updated.rowcount == 1


def retryable_orphan_provider_cancellations(
    output_json: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
) -> tuple[bool, list[tuple[str, str, dict[str, Any]]]]:
    """读取镜像一致的 orphan debt；结构异常时返回不可执行。"""

    try:
        state = _orphan_cancellation_state(output_json, metrics)
    except VideoTaskCheckpointConflict:
        return False, []
    pending: list[tuple[str, str, dict[str, Any]]] = []
    generations = state["generations"]
    for generation in sorted(generations):
        tasks = generations[generation]["tasks"]
        for task_key in sorted(tasks):
            record = tasks[task_key]
            if not isinstance(record, dict):
                return False, []
            provider_task_id = record.get("provider_task_id")
            if (
                not isinstance(provider_task_id, str)
                or not provider_task_id
                or task_key != _provider_task_identity(provider_task_id)
            ):
                return False, []
            if record.get("retryable") is True:
                pending.append((generation, task_key, dict(record)))
    if bool(state.get("pending")) != bool(pending):
        return False, []
    return True, pending


async def persist_locked_orphan_provider_cancellation_result(
    db: Any,
    job: VideoGenerationJob,
    *,
    generation: str,
    task_key: str,
    expected_record: dict[str, Any],
    result: VideoCancellationResult,
) -> bool:
    """按 job version CAS 更新一个 orphan 取消记录，不触碰当前权威 ID。"""

    current_output = dict(job.output_json or {})
    current_metrics = dict(job.metrics or {})
    try:
        state = _orphan_cancellation_state(
            current_output,
            current_metrics,
            provider_task_id=result.provider_task_id,
        )
    except VideoTaskCheckpointConflict:
        return False
    generation_record = state["generations"].get(generation)
    tasks = generation_record.get("tasks") if isinstance(generation_record, dict) else None
    if not isinstance(tasks, dict):
        return False
    current_record = tasks.get(task_key)
    if (
        not isinstance(current_record, dict)
        or current_record != expected_record
        or current_record.get("retryable") is not True
        or str(current_record.get("provider_task_id") or "") != result.provider_task_id
    ):
        return False
    tasks[task_key] = provider_cancellation_record(result, previous=current_record)
    output_json, metrics = _write_orphan_cancellation_state(
        current_output,
        current_metrics,
        state,
    )
    expected_version = int(job.version or 0)
    updated = cast(
        CursorResult[Any],
        await db.execute(
            update(VideoGenerationJob)
            .where(
                VideoGenerationJob.job_id == job.job_id,
                VideoGenerationJob.status == job.status,
                VideoGenerationJob.version == expected_version,
            )
            .values(
                output_json=output_json,
                metrics=metrics,
                version=VideoGenerationJob.version + 1,
            )
        ),
    )
    await db.flush()
    return updated.rowcount == 1


def retryable_provider_cancellation_expression():
    """返回数据库可下推的 JSON debt 过滤条件。"""
    return (
        VideoGenerationJob.output_json[PROVIDER_CANCELLATION_KEY]["retryable"]
        .as_boolean()
        .is_(True)
    )


def retryable_orphan_provider_cancellation_expression():
    """返回 orphan debt 顶层 pending 摘要过滤条件。"""

    return (
        VideoGenerationJob.output_json[ORPHAN_PROVIDER_CANCELLATIONS_KEY]["pending"]
        .as_boolean()
        .is_(True)
    )


def provider_task_id_authority(
    output_json: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
) -> tuple[bool, str | None]:
    """解析本地取消状态中的唯一权威 Provider 任务 ID。"""

    output_record = (output_json or {}).get(PROVIDER_CANCELLATION_KEY)
    metrics_record = (metrics or {}).get(PROVIDER_CANCELLATION_KEY)
    if (
        isinstance(output_record, dict)
        and isinstance(metrics_record, dict)
        and output_record != metrics_record
    ):
        return False, None
    task_ids: set[str] = set()
    for state in (output_json, metrics):
        if not isinstance(state, dict):
            continue
        raw_task_id = state.get("provider_task_id")
        if raw_task_id is not None and str(raw_task_id):
            task_ids.add(str(raw_task_id))
        record = state.get(PROVIDER_CANCELLATION_KEY)
        if isinstance(record, dict):
            raw_debt_task_id = record.get("provider_task_id")
            if raw_debt_task_id is not None and str(raw_debt_task_id):
                task_ids.add(str(raw_debt_task_id))
    if len(task_ids) > 1:
        return False, None
    return True, next(iter(task_ids), None)


def _merge_provider_cancellation(
    output_json: dict[str, Any] | None,
    metrics: dict[str, Any] | None,
    provider_task_id: str,
    record: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    merged_output = dict(output_json or {})
    merged_metrics = dict(metrics or {})
    merged_output["provider_task_id"] = provider_task_id
    merged_metrics["provider_task_id"] = provider_task_id
    merged_output[PROVIDER_CANCELLATION_KEY] = dict(record)
    merged_metrics[PROVIDER_CANCELLATION_KEY] = dict(record)
    return merged_output, merged_metrics


def _safe_attempts(previous: dict[str, Any] | None) -> int:
    try:
        return max(0, int((previous or {}).get("attempts", 0)))
    except (TypeError, ValueError):
        return 0


def _confirmed_result_must_be_preserved(
    previous: dict[str, Any] | None,
    result: VideoCancellationResult,
) -> bool:
    if not previous:
        return False
    if str(previous.get("provider_task_id") or "") != result.provider_task_id:
        return False
    previous_outcome = str(previous.get("outcome") or "")
    if previous_outcome == VideoCancellationOutcome.CANCELED.value:
        return result.outcome is not VideoCancellationOutcome.CANCELED
    return (
        previous_outcome == VideoCancellationOutcome.UNSUPPORTED.value
        and result.outcome is VideoCancellationOutcome.FAILED
    )
