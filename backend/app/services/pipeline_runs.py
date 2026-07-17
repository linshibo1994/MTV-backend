"""流水线人工重新提取与失败后显式重试。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from uuid import UUID, uuid5

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

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
    build_pipeline_outbox_payload,
)
from app.services.task_dispatch import enqueue_task

_RUN_ID_NAMESPACE = UUID("681d22d7-73fc-43bf-9706-b496b4e9d85c")
_ACTIVE_STATUSES = ("pending", "retry", "running", "ready")


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


async def _acquire_script_lock(db: AsyncSession, script_id: str) -> None:
    """串行化同一剧本的检查与创建，避免并发产生两个活动 run。"""
    await db.execute(
        select(
            func.pg_advisory_xact_lock(
                func.hashtext("pipeline_trigger"),
                func.hashtext(script_id),
            )
        )
    )


async def _load_idempotent_result(
    db: AsyncSession,
    *,
    run_id: str,
    request_fingerprint: str,
) -> PipelineRunOut | None:
    result = await db.execute(
        select(TaskDispatchOutbox).where(
            TaskDispatchOutbox.task_type == PIPELINE_DAG_TASK_TYPE,
            TaskDispatchOutbox.job_id == run_id,
        )
    )
    outbox = result.scalar_one_or_none()
    if outbox is None:
        return None
    payload = outbox.payload or {}
    if payload.get("request_fingerprint") != request_fingerprint:
        raise ConflictError("Idempotency-Key 已用于不同的流水线请求")
    jobs_result = await db.execute(
        select(PipelineJob).where(PipelineJob.run_id == run_id)
    )
    status = _derive_run_status(list(jobs_result.scalars().all()))
    return PipelineRunOut(
        script_id=payload["script_id"],
        episode_id=payload["episode_id"],
        run_id=run_id,
        dispatch_id=outbox.id,
        reservation_id=payload["reservation_id"],
        mode=PipelineRunMode(payload["mode"]),
        retry_of_run_id=payload.get("retry_of_run_id"),
        attempt=int(payload["pipeline_attempt"]),
        input_version=int(payload["input_version"]),
        input_client_save_seq=int(payload["input_client_save_seq"]),
        estimated=int(payload["estimated"]),
        status=status,
        idempotent_replay=True,
    )


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
    episode = await db.get(
        Episode,
        episode_id,
        populate_existing=True,
        with_for_update=True,
    )
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
    """只检查各阶段最新 attempt；历史失败行始终保持不可变。"""
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
    source_inputs = {
        (job.input_version, job.input_client_save_seq) for job in latest.values()
    }
    if source_inputs != {(input_version, input_client_save_seq)}:
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
        replay = await _load_idempotent_result(
            db,
            run_id=run_id,
            request_fingerprint=fingerprint,
        )
        if replay is not None:
            await db.commit()
            return replay

        episode = await _reload_episode(
            db,
            script_id=script.id,
            episode_id=episode_id,
        )
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
