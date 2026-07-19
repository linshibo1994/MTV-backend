"""流水线人工重新提取与失败后显式重试。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import cast
from uuid import UUID, uuid5

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.errors import ConflictError, ForbiddenError, InsufficientCreditsError, NotFoundError
from app.core.ids import new_id
from app.models.credit import CreditAccount
from app.models.episode import Episode
from app.models.pipeline_job import PipelineJob
from app.models.script import Script
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.user import User
from app.schemas.pipeline_runs import PipelineRunCreateIn, PipelineRunMode, PipelineRunOut
from app.services.audit import best_effort_log as audit_log
from app.services.credits import reserve
from app.services.pipeline import STAGE_ORDER, get_stage, set_stage
from app.services.pipeline.orchestrator import (
    PIPELINE_DAG_TASK_TYPE,
    _estimate_cost,
    acquire_pipeline_advisory_lock,
    build_pipeline_outbox_payload,
)
from app.services.task_dispatch import enqueue_task

_RUN_ID_NAMESPACE = UUID("681d22d7-73fc-43bf-9706-b496b4e9d85c")
_ACTIVE_STATUSES = ("pending", "retry", "running", "ready")
_PIPELINE_LEAF_STAGES = frozenset({"enrich", "parents"})
_PIPELINE_NONLEAF_STATUSES = frozenset(
    {"pending", "running", "done", "failed", "skipped"}
)
_PIPELINE_LEAF_STATUSES = _PIPELINE_NONLEAF_STATUSES | {"ready"}


def _request_fingerprint(body: PipelineRunCreateIn) -> str:
    payload = body.model_dump(mode="json")
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _idempotency_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _run_id_for_request(
    *, script_id: str, episode_id: str, actor_user_id: str, idempotency_key: str
) -> str:
    """用业务作用域生成稳定 UUID；不把原始幂等键写入数据库。"""
    scoped_key = "\x1f".join((script_id, episode_id, actor_user_id, idempotency_key))
    return str(uuid5(_RUN_ID_NAMESPACE, scoped_key))


def _pipeline_job_history_is_valid(
    jobs: Sequence[PipelineJob],
    *,
    script_id: str,
    episode_id: str,
    run_id: str,
    dispatch_id: str,
    baseline_attempt: int,
    input_version: int,
    input_client_save_seq: int,
) -> bool:
    """校验同一 run 的 attempt 历史与 worker 可达的 DAG 状态。"""
    attempts_by_stage: dict[str, dict[int, PipelineJob]] = {
        stage: {} for stage in STAGE_ORDER
    }
    for job in jobs:
        stage_attempts = attempts_by_stage.get(job.stage)
        if (
            stage_attempts is None
            or job.script_id != script_id
            or job.episode_id != episode_id
            or job.run_id != run_id
            or job.dispatch_id != dispatch_id
            or job.input_version != input_version
            or job.input_client_save_seq != input_client_save_seq
            or type(job.attempt) is not int
            or job.attempt < baseline_attempt
            or job.attempt in stage_attempts
        ):
            return False
        stage_attempts[job.attempt] = job

    latest: dict[str, PipelineJob] = {}
    has_execution_evidence: dict[str, bool] = {}
    for stage, stage_attempts in attempts_by_stage.items():
        if not stage_attempts:
            return False
        if (
            min(stage_attempts) != baseline_attempt
            or len(stage_attempts) != max(stage_attempts) - baseline_attempt + 1
        ):
            return False

        ordered_attempts = sorted(stage_attempts)
        for attempt in ordered_attempts[:-1]:
            historical = stage_attempts[attempt]
            if historical.status != "failed" or historical.retryable is not True:
                return False
        current = stage_attempts[ordered_attempts[-1]]
        allowed_statuses = (
            _PIPELINE_LEAF_STATUSES
            if stage in _PIPELINE_LEAF_STAGES
            else _PIPELINE_NONLEAF_STATUSES
        )
        if current.status not in allowed_statuses:
            return False
        latest[stage] = current
        has_execution_evidence[stage] = (
            stage_attempts[baseline_attempt].status != "pending"
            or ordered_attempts[-1] > baseline_attempt
        )

    split_status = latest["split"].status
    extract_status = latest["extract"].status
    merge_status = latest["merge"].status
    if has_execution_evidence["extract"] and split_status != "done":
        return False
    if has_execution_evidence["merge"] and (
        split_status != "done" or extract_status != "done"
    ):
        return False
    prerequisites_done = (
        split_status == "done"
        and extract_status == "done"
        and merge_status == "done"
    )
    for leaf in _PIPELINE_LEAF_STAGES:
        if has_execution_evidence[leaf] and not prerequisites_done:
            return False

    leaf_done = {stage for stage in _PIPELINE_LEAF_STAGES if latest[stage].status == "done"}
    if leaf_done and leaf_done != _PIPELINE_LEAF_STAGES:
        return False
    return True


async def _acquire_script_lock(db: AsyncSession, script_id: str) -> None:
    """先锁定真实父行，再串行化同一剧本的人工 run 创建。"""
    locked_script_id = (
        await db.execute(
            select(Script.id)
            .where(Script.id == script_id)
            .with_for_update(read=True, key_share=True)
        )
    ).scalar_one_or_none()
    if locked_script_id is None:
        raise NotFoundError("剧本不存在")
    await acquire_pipeline_advisory_lock(db, script_id)


async def _load_idempotent_result(
    db: AsyncSession,
    *,
    run_id: str,
    request_fingerprint: str,
    script_id: str,
    episode_id: str,
    body: PipelineRunCreateIn,
    requested_by_user_id: str,
    idempotency_key_hash: str,
) -> PipelineRunOut | None:
    jobs_result = await db.execute(
        select(PipelineJob)
        .where(PipelineJob.run_id == run_id)
        .order_by(PipelineJob.stage.asc(), PipelineJob.attempt.asc(), PipelineJob.id.asc())
        .options(noload(PipelineJob.script))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    jobs = list(jobs_result.scalars().all())
    outbox_result = await db.execute(
        select(TaskDispatchOutbox)
        .where(
            TaskDispatchOutbox.task_type == PIPELINE_DAG_TASK_TYPE,
            TaskDispatchOutbox.job_id == run_id,
        )
        .order_by(TaskDispatchOutbox.attempt.asc(), TaskDispatchOutbox.id.asc())
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    outboxes = list(outbox_result.scalars().all())
    if not outboxes and not jobs:
        return None
    if len(outboxes) != 1 or not jobs:
        raise ConflictError("Idempotency-Key 对应的流水线运行已失效，请使用新键重试")
    outbox = outboxes[0]
    payload = outbox.payload
    if not isinstance(payload, dict):
        raise ConflictError("Idempotency-Key 对应的流水线运行已失效，请使用新键重试")
    if payload.get("request_fingerprint") != request_fingerprint:
        raise ConflictError("Idempotency-Key 已用于不同的流水线请求")

    pipeline_attempt = payload.get("pipeline_attempt")
    input_version = payload.get("input_version")
    input_client_save_seq = payload.get("input_client_save_seq")
    estimated = payload.get("estimated")
    reservation_id = payload.get("reservation_id")
    scalar_payload_is_valid = (
        type(pipeline_attempt) is int
        and pipeline_attempt > 0
        and type(input_version) is int
        and input_version > 0
        and type(input_client_save_seq) is int
        and input_client_save_seq >= 0
        and type(estimated) is int
        and estimated > 0
        and isinstance(reservation_id, str)
        and bool(reservation_id)
    )
    expected_pipeline_attempt_is_valid = (
        pipeline_attempt == 1
        if body.mode is PipelineRunMode.MANUAL_REEXTRACT
        else type(pipeline_attempt) is int and pipeline_attempt > 0
    )
    validated_attempt = cast(int, pipeline_attempt)
    validated_input_version = cast(int, input_version)
    validated_input_client_save_seq = cast(int, input_client_save_seq)
    job_history_is_valid = scalar_payload_is_valid and _pipeline_job_history_is_valid(
        jobs,
        script_id=script_id,
        episode_id=episode_id,
        run_id=run_id,
        dispatch_id=outbox.id,
        baseline_attempt=validated_attempt,
        input_version=validated_input_version,
        input_client_save_seq=validated_input_client_save_seq,
    )
    if (
        not scalar_payload_is_valid
        or not expected_pipeline_attempt_is_valid
        or outbox.status not in {"pending", "retry", "running", "published"}
        or outbox.attempt != body.episode_version
        or payload.get("script_id") != script_id
        or payload.get("episode_id") != episode_id
        or payload.get("run_id") != run_id
        or payload.get("dispatch_id") != outbox.id
        or payload.get("mode") != body.mode.value
        or payload.get("retry_of_run_id") != body.failed_run_id
        or payload.get("requested_by_user_id") != requested_by_user_id
        or payload.get("idempotency_key_hash") != idempotency_key_hash
        or input_version != body.episode_version
        or input_client_save_seq != body.client_save_seq
        or not job_history_is_valid
    ):
        raise ConflictError("Idempotency-Key 对应的流水线运行已失效，请使用新键重试")
    validated_estimated = cast(int, estimated)
    validated_reservation_id = cast(str, reservation_id)
    status = _derive_run_status(jobs)
    try:
        replay = PipelineRunOut(
            script_id=script_id,
            episode_id=episode_id,
            run_id=run_id,
            dispatch_id=outbox.id,
            reservation_id=validated_reservation_id,
            mode=body.mode,
            retry_of_run_id=body.failed_run_id,
            attempt=validated_attempt,
            input_version=body.episode_version,
            input_client_save_seq=body.client_save_seq,
            estimated=validated_estimated,
            status=status,
            idempotent_replay=True,
        )
    except (KeyError, TypeError, ValueError):
        raise ConflictError(
            "Idempotency-Key 对应的流水线运行已失效，请使用新键重试"
        ) from None
    return replay


def _derive_run_status(jobs: Sequence[PipelineJob]) -> str:
    """按各阶段最新 attempt 推导 run 状态，避免重放返回过时的 pending。"""
    latest: dict[str, PipelineJob] = {}
    for job in jobs:
        current = latest.get(job.stage)
        if current is None or job.attempt > current.attempt:
            latest[job.stage] = job
    statuses = {job.status for job in latest.values()}
    if "failed" in statuses:
        return "failed"
    if statuses and statuses == {"done"}:
        return "done"
    if statuses and statuses <= {"done", "skipped"} and "skipped" in statuses:
        return "skipped"
    if statuses & {"running", "ready"}:
        return "running"
    return "pending"


async def _reload_episode(
    db: AsyncSession, *, script_id: str, episode_id: str
) -> Episode:
    episode = (
        await db.execute(
            select(Episode)
            .where(Episode.episode_id == episode_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if episode is None:
        raise NotFoundError("分集不存在")
    if episode.script_id != script_id:
        raise ForbiddenError("无权访问该分集")
    return episode


async def _find_active_run(db: AsyncSession, script_id: str) -> str | None:
    """阶段状态按剧本共享，因此同一剧本任意分集只允许一个活动 run。"""
    result = await db.execute(
        select(PipelineJob).where(PipelineJob.script_id == script_id)
    )
    latest: dict[tuple[str, str], PipelineJob] = {}
    for job in result.scalars().all():
        key = (job.run_id, job.stage)
        current = latest.get(key)
        if current is None or job.attempt > current.attempt:
            latest[key] = job
    runs: dict[str, list[PipelineJob]] = {}
    for job in latest.values():
        runs.setdefault(job.run_id, []).append(job)
    for run_id, jobs in runs.items():
        if any(job.status in {"running", "retry"} for job in jobs):
            return run_id
        # 失败 DAG 会留下未执行阶段为 pending；只要最新阶段中已有失败，该 run
        # 就是可供用户显式重试的终态，不能被这些遗留 pending 误判为活动运行。
        if any(job.status == "failed" for job in jobs):
            continue
        if any(job.status in _ACTIVE_STATUSES for job in jobs):
            return run_id
    return None


async def _load_retry_source(
    db: AsyncSession, *, failed_run_id: str, script_id: str, episode_id: str
) -> list[PipelineJob]:
    result = await db.execute(
        select(PipelineJob)
        .where(PipelineJob.run_id == failed_run_id)
        .order_by(PipelineJob.stage.asc(), PipelineJob.attempt.asc())
    )
    jobs = list(result.scalars().all())
    if not jobs:
        raise NotFoundError("失败流水线不存在")
    if any(job.script_id != script_id or job.episode_id != episode_id for job in jobs):
        raise ForbiddenError("失败流水线不属于当前剧本分集")
    return jobs


def _validate_retry_source(
    jobs: Sequence[PipelineJob], *, input_version: int, input_client_save_seq: int
) -> int:
    """验证完整 worker 历史后，返回新 run 的统一 baseline attempt。"""
    if not jobs or any(type(job.attempt) is not int for job in jobs):
        raise ConflictError("失败流水线历史已失效，不能显式重试")
    first = jobs[0]
    first_episode_id = first.episode_id
    first_input_client_save_seq = first.input_client_save_seq
    if first_episode_id is None or first_input_client_save_seq is None:
        raise ConflictError("失败流水线历史已失效，不能显式重试")
    baseline_attempt = min(job.attempt for job in jobs)
    if not _pipeline_job_history_is_valid(
        jobs,
        script_id=first.script_id,
        episode_id=first_episode_id,
        run_id=first.run_id,
        dispatch_id=first.dispatch_id,
        baseline_attempt=baseline_attempt,
        input_version=first.input_version,
        input_client_save_seq=first_input_client_save_seq,
    ):
        raise ConflictError("失败流水线历史已失效，不能显式重试")

    latest: dict[str, PipelineJob] = {}
    for job in jobs:
        current = latest.get(job.stage)
        if current is None or job.attempt > current.attempt:
            latest[job.stage] = job
    failed = [job for job in latest.values() if job.status == "failed"]
    if not failed:
        raise ConflictError("指定流水线不是失败终态，不能显式重试")
    if any(job.retryable is not True for job in failed):
        raise ConflictError("指定流水线包含不可重试失败")
    if (first.input_version, first_input_client_save_seq) != (
        input_version,
        input_client_save_seq,
    ):
        raise ConflictError("失败流水线输入版本已过期，请手动重新提取")
    return max(job.attempt for job in jobs) + 1


async def _load_credit_account(db: AsyncSession, script: Script) -> CreditAccount:
    result = await db.execute(
        select(CreditAccount).where(
            CreditAccount.owner_type == "user",
            CreditAccount.owner_id == script.owner_user_id,
        )
    )
    account = result.scalar_one_or_none()
    if account is None:
        raise InsufficientCreditsError("用户无积分账户")
    return account


async def _reset_pipeline_stages(
    db: AsyncSession,
    *,
    script_id: str,
    input_version: int,
    input_client_save_seq: int,
) -> None:
    for stage in STAGE_ORDER:
        row = await get_stage(db, script_id, stage)
        row.status = "pending"
        row.detail = None
        row.current = None
        row.total = None
        row.error = None
    await set_stage(
        db,
        script_id,
        "split",
        status="running",
        detail={
            "input_version": input_version,
            "input_client_save_seq": input_client_save_seq,
        },
    )


async def create_pipeline_run(
    db: AsyncSession,
    *,
    script: Script,
    episode_id: str,
    actor: User,
    body: PipelineRunCreateIn,
    idempotency_key: str,
) -> PipelineRunOut:
    """原子创建人工 run、五阶段 attempt、积分预约和可靠 outbox。"""
    fingerprint = _request_fingerprint(body)
    run_id = _run_id_for_request(
        script_id=script.id,
        episode_id=episode_id,
        actor_user_id=actor.user_id,
        idempotency_key=idempotency_key,
    )
    try:
        await _acquire_script_lock(db, script.id)
        episode = await _reload_episode(
            db,
            script_id=script.id,
            episode_id=episode_id,
        )
        replay = await _load_idempotent_result(
            db,
            run_id=run_id,
            request_fingerprint=fingerprint,
            script_id=script.id,
            episode_id=episode_id,
            body=body,
            requested_by_user_id=actor.user_id,
            idempotency_key_hash=_idempotency_hash(idempotency_key),
        )
        if replay is not None:
            await db.commit()
            return replay

        if (
            episode.episode_version != body.episode_version
            or episode.client_save_seq != body.client_save_seq
        ):
            raise ConflictError("分集版本已变化，请刷新后重新操作")

        active_run_id = await _find_active_run(db, script.id)
        if active_run_id is not None:
            raise ConflictError(f"流水线运行中，活动 run_id={active_run_id}")

        pipeline_attempt = 1
        retry_of_run_id: str | None = None
        if body.mode is PipelineRunMode.RETRY_FAILED:
            retry_of_run_id = body.failed_run_id
            assert retry_of_run_id is not None
            source_jobs = await _load_retry_source(
                db,
                failed_run_id=retry_of_run_id,
                script_id=script.id,
                episode_id=episode_id,
            )
            pipeline_attempt = _validate_retry_source(
                source_jobs,
                input_version=episode.episode_version,
                input_client_save_seq=episode.client_save_seq,
            )

        account = await _load_credit_account(db, script)
        estimated = await _estimate_cost(db, script.id)
        reservation_id = await reserve(
            db,
            account_id=account.id,
            source_type="pipeline",
            source_id=run_id,
            estimated=estimated,
            actor_user_id=actor.user_id,
            actor_username=actor.username,
            commit=False,
        )
        payload = {
            **build_pipeline_outbox_payload(
                script_id=script.id,
                episode_id=episode_id,
                input_version=episode.episode_version,
                input_client_save_seq=episode.client_save_seq,
                reservation_id=reservation_id,
                estimated=estimated,
            ),
            "mode": body.mode.value,
            "retry_of_run_id": retry_of_run_id,
            "pipeline_attempt": pipeline_attempt,
            "requested_by_user_id": actor.user_id,
            "idempotency_key_hash": _idempotency_hash(idempotency_key),
            "request_fingerprint": fingerprint,
        }
        outbox = await enqueue_task(
            db,
            task_type=PIPELINE_DAG_TASK_TYPE,
            job_id=run_id,
            attempt=episode.episode_version,
            payload=payload,
        )
        for stage in STAGE_ORDER:
            db.add(
                PipelineJob(
                    id=new_id(),
                    script_id=script.id,
                    episode_id=episode_id,
                    stage=stage,
                    dispatch_id=outbox.id,
                    run_id=run_id,
                    status="pending",
                    input_version=episode.episode_version,
                    input_client_save_seq=episode.client_save_seq,
                    attempt=pipeline_attempt,
                )
            )
        await _reset_pipeline_stages(
            db,
            script_id=script.id,
            input_version=episode.episode_version,
            input_client_save_seq=episode.client_save_seq,
        )
        await audit_log(
            db,
            actor=actor,
            action=f"pipeline_{body.mode.value}",
            resource_type="pipeline_run",
            resource_id=run_id,
            before={"run_id": retry_of_run_id} if retry_of_run_id else None,
            after={
                "script_id": script.id,
                "episode_id": episode_id,
                "run_id": run_id,
                "input_version": episode.episode_version,
                "input_client_save_seq": episode.client_save_seq,
                "attempt": pipeline_attempt,
                "reservation_id": reservation_id,
                "dispatch_id": outbox.id,
            },
        )
        await db.flush()
        await db.commit()
    except Exception:
        await db.rollback()
        raise

    return PipelineRunOut(
        script_id=script.id,
        episode_id=episode_id,
        run_id=run_id,
        dispatch_id=outbox.id,
        reservation_id=reservation_id,
        mode=body.mode,
        retry_of_run_id=retry_of_run_id,
        attempt=pipeline_attempt,
        input_version=episode.episode_version,
        input_client_save_seq=episode.client_save_seq,
        estimated=estimated,
    )
